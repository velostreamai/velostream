// Integration Tests - Require running Kafka
// These tests interact with actual Kafka instances and may be slow

// Basic integration tests
pub mod builder_pattern_test;
pub mod emit_functionality_test;
pub mod execution_engine_test;
pub mod failure_recovery_test;
pub mod job_multi_source_sink_test;
pub mod kafka_advanced_test;
pub mod kafka_basic_test;
pub mod sql;
pub mod sql_integration_test;
pub mod sql_metrics_integration_test;
pub mod sql_parser_comprehensive_test;
pub mod table;
pub mod timeout_config_test;
pub mod transactions_test;

// CTAS integration tests with modern APIs
pub mod ctas_performance_test;
pub mod ctas_sql_integration_test;

// Kafka serialization and validation tests
pub mod kafka_configurable_serialization_test;
pub mod post_cleanup_validation_test;

// Datasource integration tests
pub mod datasource;

// Re-export common test utilities from the correct path
pub use super::unit::common::*;
