//! Unit-level Performance Benchmarks
//!
//! This module contains component-specific benchmarks for individual Velostream components.
//! Each module focuses on a specific component's performance characteristics.

// Comprehensive SQL benchmarks (consolidates all SQL features)
pub mod comprehensive_sql_benchmarks;

// ROWS WINDOW and EMIT CHANGES SQL-driven performance benchmarks
pub mod rows_window_emit_changes_sql_benchmarks;

// Time-based window SQL-driven performance benchmarks (TUMBLING, SLIDING, SESSION)
pub mod time_window_sql_benchmarks;

// Specialized benchmarks
pub mod financial_precision;
pub mod kafka_configurations;
pub mod query_processing;
pub mod serialization_formats;
pub mod sql_execution;

// Re-export commonly used benchmarks
pub use comprehensive_sql_benchmarks::*;
pub use financial_precision::*;
pub use kafka_configurations::*;
pub use query_processing::*;
pub use serialization_formats::*;
pub use sql_execution::*;
