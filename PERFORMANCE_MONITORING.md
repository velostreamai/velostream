# FerrisStreams Performance Monitoring

**Phase 1: Statistics & Monitoring Infrastructure**

This document describes the comprehensive performance monitoring system implemented for the FerrisStreams SQL engine. The monitoring infrastructure provides real-time visibility into query execution performance, memory usage, and system health.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Core Components](#core-components)
- [Usage Guide](#usage-guide)
- [Metrics Reference](#metrics-reference)
- [Production Integration](#production-integration)
- [Health Monitoring](#health-monitoring)
- [Troubleshooting](#troubleshooting)
- [Performance Targets](#performance-targets)

## Overview

The FerrisStreams Performance Monitoring system tracks query execution across the entire SQL engine pipeline, providing insights into:

- **Query Execution Time**: Total time and per-processor breakdown
- **Memory Usage**: Allocated memory, peak usage, and component-specific tracking
- **Throughput**: Records and bytes processed per second
- **System Health**: Automated health checks with configurable thresholds
- **Query Patterns**: Analysis of query types and performance characteristics

### Key Benefits

- **Zero-Overhead When Disabled**: Monitoring is completely optional
- **Production-Ready**: Prometheus metrics export for monitoring dashboards
- **Real-Time Health Checks**: Automated performance issue detection
- **Processor-Level Granularity**: Track performance down to individual SQL processors
- **Thread-Safe**: Designed for high-performance multi-threaded environments

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Performance Monitor                       │
├─────────────────────────────────────────────────────────────┤
│  ┌───────────────┐  ┌──────────────┐  ┌─────────────────┐   │
│  │ QueryTracker  │  │ Statistics   │  │ Health Monitor  │   │
│  │               │  │ Collector    │  │                 │   │
│  └───────────────┘  └──────────────┘  └─────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                               │                                
                               ▼                                
┌─────────────────────────────────────────────────────────────┐
│                    Metrics Collection                        │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌────────────────┐  ┌─────────────────┐  │
│  │ Memory       │  │ Throughput     │  │ Processor       │  │
│  │ Metrics      │  │ Metrics        │  │ Metrics         │  │
│  └──────────────┘  └────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                               │                                
                               ▼                                
┌─────────────────────────────────────────────────────────────┐
│                   Query Processors                          │
├─────────────────────────────────────────────────────────────┤
│  SELECT │ JOIN │ WINDOW │ GROUP BY │ INSERT │ UPDATE │ ...   │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. PerformanceMonitor

The main interface for performance tracking.

```rust
use ferrisstreams::ferris::sql::execution::performance::PerformanceMonitor;

// Create a performance monitor
let monitor = PerformanceMonitor::new();

// Start tracking a query
let mut tracker = monitor.start_query_tracking("SELECT * FROM users");

// Add performance data
tracker.add_records_processed(1000);
tracker.add_bytes_processed(50000);

// Finish tracking and get results
let performance = monitor.finish_query_tracking(tracker);

println!("Query processed {} records in {}ms", 
    performance.records_processed,
    performance.execution_time.as_millis()
);
```

### 2. QueryTracker

Tracks individual query lifecycle and processor timing.

```rust
// Start processor-specific tracking
tracker.start_processor("SelectProcessor");
// ... execute SELECT logic ...

tracker.start_processor("JoinProcessor"); 
// ... execute JOIN logic ...

// Update memory metrics
let memory_metrics = MemoryMetrics {
    allocated_bytes: 1024 * 1024, // 1MB
    peak_memory_bytes: 2 * 1024 * 1024, // 2MB peak
    group_by_memory_bytes: 512 * 1024, // 512KB for GROUP BY
    ..Default::default()
};
tracker.update_memory_metrics(memory_metrics);
```

### 3. StatisticsCollector

Provides advanced analytics and query pattern analysis.

```rust
// Get recent performance statistics
let window_stats = monitor.get_window_statistics(Duration::from_secs(300)); // 5 minutes

if let Some(stats) = window_stats {
    println!("Average query time: {}ms", stats.avg_execution_time.as_millis());
    println!("Throughput: {:.1} records/sec", stats.throughput.records_per_second);
    println!("P95 latency: {}ms", stats.p95_execution_time.as_millis());
}

// Get top query patterns
let patterns = monitor.get_top_query_patterns(10);
for (pattern, stats) in patterns {
    println!("Pattern: {} - {} executions", pattern, stats.execution_count);
}
```

## Usage Guide

### Basic Usage

```rust
use ferrisstreams::ferris::sql::execution::performance::PerformanceMonitor;
use ferrisstreams::ferris::sql::execution::processors::QueryProcessor;
use std::sync::Arc;

// Create performance monitor (shared across threads)
let monitor = Arc::new(PerformanceMonitor::new());

// Set up processor context with monitoring
let mut context = ProcessorContext::new("query_1");
context.set_performance_monitor(Arc::clone(&monitor));

// Process query with monitoring
let result = QueryProcessor::process_query_with_monitoring(
    &query,
    &record, 
    &mut context,
    Some(&monitor)
)?;
```

### Integration with StreamExecutionEngine

```rust
use ferrisstreams::ferris::sql::execution::StreamExecutionEngine;
use ferrisstreams::ferris::sql::execution::performance::PerformanceMonitor;

// Create engine with performance monitoring
let monitor = Arc::new(PerformanceMonitor::new());
let mut engine = StreamExecutionEngine::new(output_sender, serialization_format);

// Enable performance monitoring
engine.set_performance_monitor(Some(Arc::clone(&monitor)));

// Execute queries - performance will be automatically tracked
engine.execute(&query, record).await?;

// Get performance metrics
let metrics = monitor.get_current_metrics();
println!("Total queries: {}", metrics.total_queries);
```

### Custom Processor Integration

For custom processors, add performance tracking:

```rust
impl CustomProcessor {
    fn process(&self, query: &StreamingQuery, record: &StreamRecord, 
               context: &mut ProcessorContext) -> Result<ProcessorResult, SqlError> {
        
        // Get performance monitor from context
        if let Some(monitor) = context.get_performance_monitor() {
            let mut tracker = monitor.start_query_tracking("Custom Query");
            tracker.start_processor("CustomProcessor");
            
            // Your processing logic here...
            let result = self.process_internal(query, record)?;
            
            tracker.add_records_processed(1);
            tracker.add_bytes_processed(record.size_estimate());
            
            monitor.finish_query_tracking(tracker);
            return Ok(result);
        }
        
        // Fallback without monitoring
        self.process_internal(query, record)
    }
}
```

## Metrics Reference

### Memory Metrics

| Metric | Description | Units |
|--------|-------------|--------|
| `allocated_bytes` | Currently allocated memory | bytes |
| `peak_memory_bytes` | Peak memory usage during query | bytes |
| `group_by_memory_bytes` | Memory used by GROUP BY operations | bytes |
| `window_memory_bytes` | Memory used by window functions | bytes |
| `join_memory_bytes` | Memory used by JOIN operations | bytes |

```rust
let memory_efficiency = metrics.memory_efficiency(bytes_processed);
println!("Memory efficiency: {:.2} bytes processed per byte allocated", memory_efficiency);
```

### Throughput Metrics

| Metric | Description | Units |
|--------|-------------|--------|
| `records_per_second` | Records processed per second | records/sec |
| `bytes_per_second` | Bytes processed per second | bytes/sec |
| `queries_per_second` | Queries executed per second | queries/sec |

### Processor Metrics

| Metric | Description | Units |
|--------|-------------|--------|
| `processor_name` | Name of the processor | string |
| `records_processed` | Total records processed | count |
| `execution_time` | Total execution time | Duration |
| `success_count` | Successful executions | count |
| `error_count` | Failed executions | count |

```rust
let efficiency = processor_metrics.efficiency(); // records per second
let success_rate = processor_metrics.success_rate(); // 0.0 to 1.0
```

## Production Integration

### Prometheus Metrics Export

```rust
// Export metrics in Prometheus format
let prometheus_output = monitor.export_prometheus_metrics();

// Example output:
// # HELP ferrisstreams_queries_total Total number of queries executed
// # TYPE ferrisstreams_queries_total counter
// ferrisstreams_queries_total 1234
//
// # HELP ferrisstreams_query_duration_seconds Query execution time
// # TYPE ferrisstreams_query_duration_seconds histogram
// ferrisstreams_query_duration_seconds_bucket{le="0.1"} 800
// ferrisstreams_query_duration_seconds_bucket{le="0.5"} 1200
```

### Grafana Dashboard Integration

The Prometheus metrics can be visualized in Grafana:

```yaml
# Example Grafana panel query
rate(ferrisstreams_queries_total[5m])  # Queries per second
histogram_quantile(0.95, ferrisstreams_query_duration_seconds)  # P95 latency
```

### Health Check Endpoint

```rust
// Check system health
let health = monitor.health_check();

match health.status {
    HealthStatus::Healthy => println!("System is performing well"),
    HealthStatus::Warning => {
        println!("Performance warnings:");
        for warning in health.warnings {
            println!("  - {}", warning);
        }
    },
    HealthStatus::Critical => {
        println!("Critical performance issues:");
        for issue in health.issues {
            println!("  - {}", issue);
        }
    }
}
```

## Health Monitoring

### Configurable Thresholds

Default health check thresholds:

| Metric | Warning Threshold | Critical Threshold |
|--------|-------------------|-------------------|
| Average Query Latency | N/A | > 1000ms |
| Throughput | < 10 records/sec | N/A |
| Memory Usage | > 1GB | N/A |

### Custom Health Checks

```rust
// Get detailed performance report
let report = monitor.get_performance_report();
println!("{}", report);

// Example output:
// =====================================
// FerrisStreams Performance Report
// =====================================
// Generated: 2025-01-24 10:30:00 UTC
// Status: Healthy
// 
// === Overall Metrics ===
// Total Queries: 1,234
// Average Execution Time: 45.2ms
// P95 Execution Time: 128.5ms
// Throughput: 127.3 records/sec
// ...
```

## Troubleshooting

### Common Issues

#### High Memory Usage

```rust
// Check memory metrics breakdown
let metrics = monitor.get_current_metrics();
println!("Memory breakdown:");
println!("  Total: {} MB", metrics.memory.allocated_bytes / 1024 / 1024);
println!("  GROUP BY: {} MB", metrics.memory.group_by_memory_bytes / 1024 / 1024);
println!("  JOIN: {} MB", metrics.memory.join_memory_bytes / 1024 / 1024);
println!("  Window: {} MB", metrics.memory.window_memory_bytes / 1024 / 1024);
```

#### Slow Queries

```rust
// Get slowest queries
let slow_queries = monitor.get_slow_queries(10);
for query_perf in slow_queries {
    println!("Query: {} - Duration: {}ms", 
        query_perf.query_text, 
        query_perf.execution_time.as_millis()
    );
    
    // Check processor breakdown
    let breakdown = query_perf.processor_time_breakdown();
    for (processor, percentage) in breakdown {
        println!("  {}: {:.1}%", processor, percentage);
    }
}
```

#### Low Throughput

```rust
// Analyze recent performance
let recent_stats = monitor.get_window_statistics(Duration::from_secs(60));
if let Some(stats) = recent_stats {
    if stats.throughput.records_per_second < 100.0 {
        println!("Low throughput detected: {:.1} records/sec", 
            stats.throughput.records_per_second);
        
        // Check for bottlenecks
        let patterns = monitor.get_top_query_patterns(5);
        for (pattern, pattern_stats) in patterns {
            println!("Pattern: {} - Avg time: {}ms", 
                pattern, 
                pattern_stats.avg_execution_time.as_millis()
            );
        }
    }
}
```

### Debug Mode

Enable detailed logging for performance analysis:

```rust
// Reset statistics for clean analysis
monitor.reset_statistics();

// Execute your queries...

// Get comprehensive report
let report = monitor.get_performance_report();
println!("{}", report);
```

## Performance Targets

Phase 1 monitoring provides baseline measurements for optimization targets:

| Component | Target | Current Baseline |
|-----------|--------|------------------|
| Simple SELECT | < 10ms | TBD |
| Complex JOIN | < 100ms | TBD |
| Window Functions | < 50ms | TBD |
| GROUP BY | < 25ms | TBD |
| Memory Usage | < 100MB per query | TBD |
| Throughput | > 10K records/sec | TBD |

These targets will be refined based on real-world usage patterns and serve as goals for subsequent optimization phases.

## Next Steps

Phase 1 provides the foundation for upcoming performance improvements:

- **Phase 2**: Hash Join Optimizations
- **Phase 3**: Cost-based Query Optimization
- **Phase 4**: Adaptive Query Execution
- **Phase 5**: Advanced State Management

The monitoring infrastructure will track the effectiveness of each optimization phase, providing data-driven insights for continuous performance improvements.