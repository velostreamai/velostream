// Integration Tests - Require running Kafka
// These tests interact with actual Kafka instances and may be slow

pub mod kafka_integration_test;
pub mod kafka_advanced_test;

// Re-export common test utilities
pub use crate::unit::common::*;