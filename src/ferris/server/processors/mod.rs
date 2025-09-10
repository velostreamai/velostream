//! Job Processors for StreamJobServer
//!
//! This module contains different job processing strategies that can be used
//! with the StreamJobServer. Each processor implements different execution
//! patterns optimized for specific use cases.

pub mod common;
pub mod simple;
pub mod transactional;

// Re-exports
pub use common::*;
pub use simple::SimpleJobProcessor;
pub use transactional::TransactionalJobProcessor;
