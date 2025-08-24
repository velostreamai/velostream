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
use crate::ferris::sql::execution::performance::{QueryStatistics, PerformanceMonitor};

// Initialize performance monitoring
let mut monitor = PerformanceMonitor::new();

// Track query execution
let stats = monitor.start_query_tracking("SELECT * FROM stream");
// ... execute query ...
let metrics = monitor.finish_query_tracking(stats);

println!("Query took: {}ms", metrics.execution_time_ms);
```
*/

pub mod metrics;
pub mod monitor;
pub mod statistics;

// Re-export public API
pub use metrics::{MemoryMetrics, PerformanceMetrics, ProcessorMetrics, ThroughputMetrics};
pub use monitor::PerformanceMonitor;
pub use statistics::{QueryStatistics, StatisticsCollector};

use std::time::{Duration, Instant};

/// Query execution performance data
#[derive(Debug, Clone)]
pub struct QueryPerformance {
    /// Query identifier for tracking
    pub query_id: String,
    /// SQL query text (truncated for logging)
    pub query_text: String,
    /// Total execution time
    pub execution_time: Duration,
    /// Time spent in each processor
    pub processor_times: std::collections::HashMap<String, Duration>,
    /// Memory usage during execution
    pub memory_metrics: MemoryMetrics,
    /// Records processed
    pub records_processed: u64,
    /// Bytes processed
    pub bytes_processed: u64,
    /// Query start timestamp
    pub started_at: Instant,
    /// Query completion timestamp
    pub completed_at: Instant,
}

impl QueryPerformance {
    /// Create new query performance tracker
    pub fn new(query_id: String, query_text: String) -> Self {
        let now = Instant::now();
        Self {
            query_id,
            query_text,
            execution_time: Duration::default(),
            processor_times: std::collections::HashMap::new(),
            memory_metrics: MemoryMetrics::default(),
            records_processed: 0,
            bytes_processed: 0,
            started_at: now,
            completed_at: now,
        }
    }

    /// Calculate throughput in records per second
    pub fn records_per_second(&self) -> f64 {
        if self.execution_time.is_zero() {
            0.0
        } else {
            self.records_processed as f64 / self.execution_time.as_secs_f64()
        }
    }

    /// Calculate throughput in bytes per second
    pub fn bytes_per_second(&self) -> f64 {
        if self.execution_time.is_zero() {
            0.0
        } else {
            self.bytes_processed as f64 / self.execution_time.as_secs_f64()
        }
    }

    /// Get processor time percentage breakdown
    pub fn processor_time_breakdown(&self) -> std::collections::HashMap<String, f64> {
        let mut breakdown = std::collections::HashMap::new();
        let total_time = self.execution_time.as_nanos() as f64;

        if total_time > 0.0 {
            for (processor, duration) in &self.processor_times {
                let percentage = (duration.as_nanos() as f64 / total_time) * 100.0;
                breakdown.insert(processor.clone(), percentage);
            }
        }

        breakdown
    }
}

/// Performance tracking token for query lifecycle
#[derive(Debug)]
pub struct QueryTracker {
    /// Query performance data
    pub performance: QueryPerformance,
    /// Current processor being tracked
    current_processor: Option<String>,
    /// Processor start time
    processor_start: Option<Instant>,
}

impl QueryTracker {
    /// Create new query tracker
    pub fn new(query_id: String, query_text: String) -> Self {
        Self {
            performance: QueryPerformance::new(query_id, query_text),
            current_processor: None,
            processor_start: None,
        }
    }

    /// Start tracking a specific processor
    pub fn start_processor(&mut self, processor_name: &str) {
        // Finish previous processor if any
        self.finish_current_processor();

        self.current_processor = Some(processor_name.to_string());
        self.processor_start = Some(Instant::now());
    }

    /// Finish tracking current processor
    pub fn finish_current_processor(&mut self) {
        if let (Some(processor), Some(start_time)) =
            (&self.current_processor, &self.processor_start)
        {
            let duration = start_time.elapsed();
            self.performance
                .processor_times
                .insert(processor.clone(), duration);
            self.current_processor = None;
            self.processor_start = None;
        }
    }

    /// Update memory metrics
    pub fn update_memory_metrics(&mut self, metrics: MemoryMetrics) {
        self.performance.memory_metrics = metrics;
    }

    /// Increment record count
    pub fn add_records_processed(&mut self, count: u64) {
        self.performance.records_processed += count;
    }

    /// Increment byte count
    pub fn add_bytes_processed(&mut self, bytes: u64) {
        self.performance.bytes_processed += bytes;
    }

    /// Finish query tracking and return performance data
    pub fn finish(mut self) -> QueryPerformance {
        // Finish any current processor
        self.finish_current_processor();

        // Calculate total execution time
        self.performance.completed_at = Instant::now();
        self.performance.execution_time = self
            .performance
            .completed_at
            .duration_since(self.performance.started_at);

        self.performance
    }
}
