//! Performance Test Suite for Velostream
//!
//! This module contains comprehensive performance benchmarks organized by category:
//!
//! ## Directory Structure
//!
//! - `analysis/` - SQL operation benchmarks organized by tier (tier1-tier4)
//! - `baseline/` - Baseline validation and strategy comparison benchmarks
//! - `common/` - Shared utilities (config, metrics, test data)
//! - `integration/` - End-to-end pipeline benchmarks
//! - `kafka/` - Kafka consumer/producer throughput benchmarks
//! - `load/` - High-throughput and stress testing
//! - `microbench/` - Low-level component profiling
//! - `regression/` - Performance regression detection tests
//! - `serialization/` - JSON/Avro format benchmarks
//! - `unit/` - Component-specific benchmarks (SQL, serialization, Kafka configs)
//!
//! Heavy benchmarks are run as examples in the performance-tests.yml workflow.
//! Hash join performance tests are located in tests/unit/sql/execution/algorithms/

// =============================================================================
// Shared Utilities
// =============================================================================

/// Common utilities for unified performance testing
pub mod common;

/// Benchmark validation utilities for metrics and record sampling
pub mod validation;

// =============================================================================
// Organized Test Categories
// =============================================================================

/// SQL operation benchmarks organized by tier (essential → specialized)
pub mod analysis;

/// Baseline validation and strategy comparison benchmarks
pub mod baseline;

/// End-to-end pipeline benchmarks
pub mod integration;

/// Kafka consumer/producer throughput benchmarks
pub mod kafka;

/// High-throughput and stress testing
pub mod load;

/// Low-level component profiling and microbenchmarks
pub mod microbench;

/// Performance regression detection tests
pub mod regression;

/// Serialization format benchmarks (JSON, Avro)
pub mod serialization;

/// Component-specific benchmarks (SQL execution, query processing)
pub mod unit;

// =============================================================================
// Legacy Support (for backward compatibility)
// =============================================================================

/// Legacy consolidated module - maintained for compatibility during transition
pub mod consolidated_mod;

// =============================================================================
// Re-exports
// =============================================================================

pub use common::{BenchmarkConfig, BenchmarkMode, MetricsCollector};
pub use consolidated_mod::*;
