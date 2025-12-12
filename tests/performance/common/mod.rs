//! Common utilities for Velostream performance testing
//!
//! This module provides shared utilities to eliminate code duplication across
//! performance tests and ensure consistent measurement approaches.

pub mod config;
pub mod metrics;
pub mod shared_container;
pub mod test_data;

// Re-export commonly used types
pub use config::{BenchmarkConfig, BenchmarkMode};
pub use metrics::{MetricsCollector, PerformanceReport};
pub use shared_container::{
    ContainerType, SharedKafkaContainer, get_shared_kafka, get_shared_kafka_with_type,
};
pub use test_data::{TestRecordConfig, generate_test_records};
