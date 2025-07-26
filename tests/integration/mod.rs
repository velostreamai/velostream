// Integration Tests - Require running Kafka
// These tests interact with actual Kafka instances and may be slow

pub mod kafka_integration_test;
pub mod kafka_advanced_test;
mod failure_recovery_test;
mod transaction_test;

// Re-export common test utilities
pub use crate::unit::common::*;