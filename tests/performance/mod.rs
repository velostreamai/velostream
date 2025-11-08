//! Performance Test Suite for Velostream
//!
//! This module contains comprehensive performance benchmarks organized by test hierarchy:
//! - `unit/`: Component-specific benchmarks for individual Velostream components
//! - `integration/`: End-to-end benchmarks testing multiple components together
//! - `load/`: High-throughput and stress testing for extreme conditions
//!
//! Heavy benchmarks are run as examples in the performance-tests.yml workflow.
//! Hash join performance tests are located in tests/unit/sql/execution/algorithms/

// Common utilities for unified performance testing (Phase 1)
pub mod common;
pub mod validation; // Benchmark validation utilities for metrics and record sampling

// Organized test hierarchy (Phase 2)
pub mod analysis; // Performance profiling and analysis
pub mod integration; // End-to-end pipeline benchmarks
pub mod load;
pub mod unit; // Component-specific benchmarks // High-throughput and stress testing

// Legacy modules (maintained for compatibility during transition)
pub mod consolidated_mod;

// WHERE clause performance benchmarks (now also in comprehensive_sql_benchmarks)
pub mod where_clause_performance_test;

// Individual performance test modules (these ARE discoverable and clickable in IDEs)
pub mod avro_decimal_roundtrip_test;
// Removed: fr082_phase1_partitioned_routing_benchmark (Phase 1, replaced by pluggable strategies)
pub mod fr082_strategy_comparison_benchmark; // FR-082 Phase 2-3: Strategy comparison (V1 vs V2)
pub mod kafka_consumer_benchmark;
pub mod microbench_job_server_profiling;
pub mod microbench_job_server_profiling_comprehensive; // Phase 3 validation: window + GROUP BY
pub mod microbench_multi_sink_write;
pub mod performance_optimization_verification;
pub mod performance_regression_test;
pub mod phase6_2_baseline_validation; // Phase 6.2: V2 baseline with SQL execution integration
pub mod week9_v1_v2_baseline_benchmarks; // Week 9 Part B: V1 vs V2 architecture baseline testing

// Re-export organized structure for easier access
pub use consolidated_mod::*;
pub use integration::*;
pub use load::*;
pub use unit::*;

// Re-export common utilities for external use
pub use common::{BenchmarkConfig, BenchmarkMode, MetricsCollector};
