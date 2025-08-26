/*!
# Performance Monitoring Module

Provides comprehensive performance statistics and monitoring for the FerrisStreams SQL engine.
This module tracks query execution metrics, memory usage, and throughput to enable
performance optimization and production monitoring.

## Features

- **Query Statistics**: Execution time, memory usage, throughput tracking
- **Processor Metrics**: Per-processor performance measurement
- **Real-time Monitoring**: Live performance data collection
- **Prometheus Integration**: Export metrics for monitoring dashboards

## Usage

```rust
use ferrisstreams::ferris::sql::execution::performance::{QueryTracker, PerformanceMonitor};

// Initialize performance monitoring
let monitor = PerformanceMonitor::new();

// Track query execution
let tracker = monitor.start_query_tracking("SELECT * FROM stream");
// ... execute query ...
let metrics = monitor.finish_query_tracking(tracker);

println!("Query took: {}ms", metrics.execution_time.as_millis());
```
*/

// Core performance tracking classes
pub mod query_performance;
pub mod query_tracker;

// Metrics collection and analysis
pub mod metrics;
pub mod monitor;
pub mod statistics;

// Re-export public API - Core Classes
pub use query_performance::QueryPerformance;

// Re-export public API - Metrics & Monitoring
pub use monitor::PerformanceMonitor;
