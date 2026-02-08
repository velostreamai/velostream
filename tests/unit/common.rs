// Common imports and re-exports for Kafka tests
// This module consolidates all commonly used imports to reduce duplication across test files

// Re-export commonly used Kafka items
pub use velostream::velostream::kafka::consumer_config::{ConsumerConfig, OffsetReset};
pub use velostream::velostream::kafka::kafka_error::KafkaClientError;
pub use velostream::velostream::kafka::performance_presets::PerformancePresets;
pub use velostream::velostream::kafka::producer_config::{
    AckMode, CompressionType, ProducerConfig,
};
pub use velostream::velostream::kafka::{
    FastConsumer, Headers, JsonSerializer, KafkaProducer, Message,
};
// Import from correct modules
pub use velostream::velostream::kafka::serialization::Serde;
pub use velostream::velostream::serialization::SerializationError;

// Re-export commonly used external crates
pub use chrono::Utc;
pub use serde::{Deserialize, Serialize};
pub use serial_test::serial;
pub use std::time::Duration;
pub use uuid::Uuid;

// Re-export test utilities and messages
pub use crate::unit::test_messages::*;
pub use crate::unit::test_utils::*;

// Re-export observability test helpers
pub use crate::unit::observability_test_helpers::*;
