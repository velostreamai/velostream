// Integration Tests - Require running Kafka
// These tests interact with actual Kafka instances and may be slow

// Alias reuse integration tests - FR-078
pub mod alias_reuse_trading_integration_test;

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
pub mod stream_job_server_processor_config_test;
pub mod table;
pub mod timeout_config_test;
pub mod transactions_test;

// CTAS integration tests with modern APIs
pub mod ctas_performance_test;
pub mod ctas_sql_integration_test;

// Kafka serialization and validation tests
pub mod kafka_configurable_serialization_test;
// Phase 2B: Kafka consumer integration tests with testcontainers
pub mod kafka;

// Datasource integration tests
pub mod datasource;

// Observability integration tests
pub mod observability_job_processor_trace_test;
pub mod observability_queue_integration_test;
pub mod observability_trace_propagation_test;
pub mod processor_trace_patterns_test;
pub mod span_structure_functional_test;
pub mod trace_attributes_comprehensive_test;
pub mod trace_chain_kafka_test;

// Event-time Kafka round-trip tests
pub mod event_time_kafka_roundtrip_test;

// Watermark implementation verification tests
pub mod watermark_debug_test;
pub mod watermark_partition_batching_verification_test;

// Re-export common test utilities from the correct path
pub use super::unit::common::*;
