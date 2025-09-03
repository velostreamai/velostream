// Performance Tests Module
//
// This module contains performance-related tests organized by component.
// Heavy benchmarks are run as examples in the performance-tests.yml workflow.
// Hash join performance tests are located in tests/unit/sql/execution/algorithms/

// Existing test modules (maintained for backward compatibility)
pub mod financial_precision_benchmark;
pub mod kafka_performance_tests;
pub mod query_performance_tests;
pub mod serialization_performance_tests;

// New consolidated performance testing framework
pub mod consolidated_mod;

// Re-export consolidated structure for easier access
pub use consolidated_mod::*;
