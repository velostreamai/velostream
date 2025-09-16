//! Common utilities for VeloStream performance testing
//!
//! This module provides shared utilities to eliminate code duplication across
//! performance tests and ensure consistent measurement approaches.

pub mod config;
pub mod metrics;
pub mod test_data;

// Re-export commonly used types
pub use config::{BenchmarkConfig, BenchmarkMode};
pub use metrics::{MetricsCollector, PerformanceReport};
pub use test_data::{generate_test_records, TestRecordConfig};
