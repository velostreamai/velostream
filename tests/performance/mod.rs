// Performance Tests Module
//
// This module contains performance-related tests organized by component.
// Heavy benchmarks are run as examples in the performance-tests.yml workflow.
// Hash join performance tests are located in tests/unit/sql/execution/algorithms/

pub mod kafka_performance_tests;
pub mod query_performance_tests;
pub mod serialization_performance_tests;
pub mod financial_precision_benchmark;
