// Performance Tests Module
//
// This module contains performance-related tests organized by component.
// Heavy benchmarks are run as examples in the performance-tests.yml workflow.
// Hash join performance tests are located in tests/unit/sql/execution/algorithms/

// Common utilities for unified performance testing
pub mod common;

// Unified benchmark modules (Phase 1 consolidation)
pub mod unified_sql_benchmarks;

// Existing test modules (maintained for backward compatibility)
pub mod financial_precision_benchmark;
pub mod kafka_performance_tests;
pub mod query_performance_tests;
pub mod serialization_performance_tests;

// Legacy benchmark modules (consolidation complete for SQL benchmarks)
// CONSOLIDATED: ferris_sql_multi_benchmarks.rs (980 lines) + ferris_sql_multi_enhanced_benchmarks.rs (1,102 lines)
//               → unified_sql_benchmarks.rs (356 lines) = 83% code reduction (1,726 lines eliminated)
pub mod transactional_processor_benchmarks;

// New consolidated performance testing framework
pub mod consolidated_mod;
mod phase_3_benchmarks;

// Re-export consolidated structure for easier access
pub use consolidated_mod::*;

// Re-export common utilities for external use
pub use common::{BenchmarkConfig, BenchmarkMode, MetricsCollector};
