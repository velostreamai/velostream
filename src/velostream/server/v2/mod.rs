//! Job Server V2: Hash-Partitioned Pipeline Architecture
//!
//! This module implements the V2 hash-partitioned architecture designed to achieve
//! 1.5M rec/sec throughput on 8 cores (65x improvement over V1: 23K rec/sec).
//!
//! ## Architecture Overview
//!
//! ```text
//!               Hash(group_key) % N
//!               ┌─────────────────┐
//! Source ──► Router              │
//!               └─────────────────┘
//!                      │
//!          ┌───────────┼───────────┐
//!          │           │           │
//!          ▼           ▼           ▼
//!     Partition 0  Partition 1  Partition N-1
//!     [State Mgr]  [State Mgr]  [State Mgr]
//!     [200K r/s]   [200K r/s]   [200K r/s]
//!          │           │           │
//!          └───────────┼───────────┘
//!                      ▼
//!                   Output
//!              N × 200K rec/sec
//! ```
//!
//! ## Key Components
//!
//! - **HashRouter**: Routes records to partitions based on GROUP BY key hashing
//! - **PartitionStateManager**: Manages query state for a single partition (lock-free)
//! - **PartitionMetrics**: Monitors throughput, queue depth, and latency per partition
//!
//! ## Implementation Phases
//!
//! - **Phase 1** (Week 3): Hash routing + partition manager → 400K rec/sec (2 cores)
//! - **Phase 2** (Week 4): Partitioned coordinator → 800K rec/sec (4 cores)
//! - **Phase 3** (Week 5): Backpressure + observability → Production-ready
//! - **Phase 4-5** (Weeks 6-8): System fields, watermarks, ROWS WINDOW, state TTL
//!
//! ## Performance Targets
//!
//! | Phase | Cores | Throughput | Scaling Efficiency |
//! |-------|-------|------------|-------------------|
//! | Phase 1 | 2 | 400K rec/sec | 95% |
//! | Phase 2 | 4 | 800K rec/sec | 90% |
//! | Phase 3-5 | 8 | 1.5M rec/sec | 85-90% |
//!
//! ## Reference
//!
//! See `docs/feature/FR-082-perf-part-2/FR-082-JOB-SERVER-V2-PARTITIONED-PIPELINE.md`

pub mod coordinator;
pub mod fan_in_strategy;
pub mod job_processor_v2;
pub mod metrics;
pub mod partition_manager;
pub mod partitioning_strategy;
pub mod prometheus_exporter;
pub mod round_robin_strategy;
pub mod smart_repartition_strategy;
pub mod sticky_partition_strategy;
pub mod strategy_config;
pub mod strategy_factory;
pub mod system_fields;
pub mod watermark;

// Re-exports for convenience
pub use coordinator::{
    BackpressureConfig, CoordinatorMetrics, PartitionedJobConfig, PartitionedJobCoordinator,
    ProcessingMode, ThrottleConfig,
};
pub use fan_in_strategy::FanInStrategy;
pub use metrics::{BackpressureState, PartitionMetrics, PartitionMetricsSnapshot};
pub use partition_manager::PartitionStateManager;
pub use partitioning_strategy::{
    AlwaysHashStrategy, PartitioningStrategy, QueryMetadata, RoutingContext,
};
pub use prometheus_exporter::PartitionPrometheusExporter;
pub use round_robin_strategy::RoundRobinStrategy;
pub use smart_repartition_strategy::SmartRepartitionStrategy;
pub use sticky_partition_strategy::StickyPartitionStrategy;
pub use strategy_config::{StrategyConfig, StrategyConfigBuilder};
pub use strategy_factory::StrategyFactory;
pub use system_fields::{
    WINDOW_END, WINDOW_START, extract_window_fields, has_window_fields, inject_window_end_field,
    inject_window_fields, inject_window_start_field,
};
pub use watermark::{WatermarkConfig, WatermarkManager, WatermarkMetrics, WatermarkStrategy};
