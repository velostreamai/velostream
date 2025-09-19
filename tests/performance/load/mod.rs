//! Load Testing Benchmarks
//!
//! High-throughput and stress testing benchmarks for validating
//! Velostream performance under extreme loads and concurrent access patterns.

pub mod concurrent_access;
pub mod high_throughput;
pub mod stress_testing;

// Re-export commonly used load test benchmarks
pub use concurrent_access::*;
pub use high_throughput::*;
pub use stress_testing::*;
