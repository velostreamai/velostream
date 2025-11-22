//! Infrastructure Performance Tests
//!
//! Low-level implementation quality tests for core Velostream components.
//! These tests validate performance of fundamental building blocks that all SQL operations depend on.
//!
//! ## Test Categories
//!
//! ### Table Operations
//! - `table_lookup.rs`: OptimizedTableImpl performance (O(1) lookups, query caching, memory efficiency)
//!
//! ### Optional: Kafka-Dependent Tests
//! For comprehensive infrastructure testing with Kafka, see `examples/performance/`:
//! - `phase4_batch_benchmark.rs` - Batch strategy optimization
//! - `datasource_performance_test.rs` - Abstraction layer overhead
//! - `json_performance_test.rs` - JSON serialization throughput
//! - `raw_bytes_performance_test.rs` - Raw byte processing
//! - `latency_performance_test.rs` - End-to-end latency
//! - `simple_async_optimization_test.rs` - Async pattern optimization
//! - `simple_zero_copy_test.rs` - Zero-copy pattern validation
//!
//! These Kafka tests are intentionally kept as examples for optional local testing.
//! See [CONSOLIDATION_PLAN.md](./CONSOLIDATION_PLAN.md) for rationale.

pub mod table_lookup;
