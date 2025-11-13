//! Job Processors for StreamJobServer
//!
//! This module contains different job processing strategies that can be used
//! with the StreamJobServer. Each processor implements different execution
//! patterns optimized for specific use cases.

pub mod common;
pub mod error_tracking_helper;
pub mod job_processor_config;
pub mod job_processor_factory;
pub mod job_processor_trait;
pub mod metrics_collector;
pub mod metrics_helper;
pub mod mock;
pub mod observability_helper;
pub mod observability_utils;
pub mod processor_registry;
pub mod profiling_helper;
pub mod simple;
pub mod transactional;

// Re-exports
pub use common::*;
pub use error_tracking_helper::ErrorTracker;
pub use job_processor_config::JobProcessorConfig;
pub use job_processor_factory::JobProcessorFactory;
pub use job_processor_trait::{JobProcessor, LifecycleState, ProcessorMetrics};
pub use metrics_collector::MetricsCollector;
pub use metrics_helper::{
    LabelHandlingConfig, MetricsPerformanceTelemetry, ProcessorMetricsHelper,
};
pub use mock::MockJobProcessor;
pub use observability_helper::ObservabilityHelper;
pub use processor_registry::{MigrationStrategy, ProcessorRegistry, RoutingStats};
pub use profiling_helper::{
    ProfilingHelper, ProfilingMetrics, ProfilingMetricsSummary, TimingScope,
};
pub use simple::SimpleJobProcessor;
pub use transactional::TransactionalJobProcessor;
