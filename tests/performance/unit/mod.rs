//! Unit-level Performance Benchmarks
//!
//! This module contains component-specific benchmarks for individual Velostream components.
//! Each module focuses on a specific component's performance characteristics.

pub mod financial_precision;
pub mod kafka_configurations;
pub mod query_processing;
pub mod serialization_formats;
pub mod sql_execution;

// Re-export commonly used benchmarks
pub use financial_precision::*;
pub use kafka_configurations::*;
pub use query_processing::*;
pub use serialization_formats::*;
pub use sql_execution::*;
