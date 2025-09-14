//! Integration-level Performance Benchmarks
//!
//! This module contains end-to-end performance benchmarks that test multiple components
//! working together, including full streaming pipelines, cross-format serialization,
//! and production scenario simulations.

pub mod resource_management;
pub mod streaming_pipeline;
pub mod transaction_processing;

// Re-export commonly used integration benchmarks
pub use resource_management::*;
pub use streaming_pipeline::*;
pub use transaction_processing::*;
