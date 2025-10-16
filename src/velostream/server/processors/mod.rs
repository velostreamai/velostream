//! Job Processors for StreamJobServer
//!
//! This module contains different job processing strategies that can be used
//! with the StreamJobServer. Each processor implements different execution
//! patterns optimized for specific use cases.

pub mod common;
pub mod error_tracking_helper;
pub mod metrics_helper;
pub mod observability_helper;
pub mod observability_utils;
pub mod simple;
pub mod transactional;

// Re-exports
pub use common::*;
pub use error_tracking_helper::ErrorTracker;
pub use metrics_helper::{
    LabelHandlingConfig, MetricsPerformanceTelemetry, ProcessorMetricsHelper,
};
pub use observability_helper::ObservabilityHelper;
pub use simple::SimpleJobProcessor;
pub use transactional::TransactionalJobProcessor;
