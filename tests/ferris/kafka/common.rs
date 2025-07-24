// Common imports and re-exports for Kafka tests
// This module consolidates all commonly used imports to reduce duplication across test files

// Re-export commonly used Kafka items
pub use ferrisstreams::ferris::kafka::{
    KafkaProducer, KafkaConsumer, ProducerBuilder, ConsumerBuilder,
    JsonSerializer, Headers, KafkaClientError, SerializationError, Serializer
};
pub use ferrisstreams::ferris::kafka::producer_config::{ProducerConfig, CompressionType, AckMode};
pub use ferrisstreams::ferris::kafka::consumer_config::{ConsumerConfig, OffsetReset};
pub use ferrisstreams::ferris::kafka::performance_presets::PerformancePresets;

// Re-export commonly used external crates
pub use serde::{Serialize, Deserialize};
pub use std::time::Duration;
pub use uuid::Uuid;
pub use serial_test::serial;
pub use chrono::Utc;

// Re-export test utilities and messages
pub use crate::ferris::kafka::test_utils::*;
pub use crate::ferris::kafka::test_messages::*;