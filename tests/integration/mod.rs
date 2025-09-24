// Integration Tests - Require running Kafka
// These tests interact with actual Kafka instances and may be slow

pub mod builder_pattern_test;
pub mod emit_functionality_test;
pub mod execution_engine_test;
pub mod failure_recovery_test;
pub mod kafka_advanced_test;
pub mod kafka_basic_test;
pub mod sql_integration_test;
pub mod table;
pub mod timeout_config_test;
pub mod transactions_test;

// Re-export common test utilities from the correct path
pub use super::unit::common::*;
