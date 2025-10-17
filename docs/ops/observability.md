# Velostream Observability Guide

## Overview

Velostream Phase 4 provides comprehensive observability through distributed tracing, metrics collection, and performance profiling. This guide covers setup, configuration, and usage of the observability infrastructure.

## Architecture

The observability system consists of three main components:

### 1. Distributed Tracing (OpenTelemetry Compatible)
- **Purpose**: Track SQL query execution and streaming operations across components
- **Implementation**: Simplified logging-based spans with structured telemetry data
- **Provider**: `TelemetryProvider` in `src/velo/observability/telemetry.rs`

### 2. Metrics Collection (Prometheus)
- **Purpose**: Collect and export SQL, streaming, and system metrics
- **Implementation**: Prometheus metrics with histograms, counters, and gauges
- **Provider**: `MetricsProvider` in `src/velo/observability/metrics.rs`

### 3. Performance Profiling
- **Purpose**: Detect bottlenecks and generate performance reports
- **Implementation**: CPU/memory monitoring with automated report generation
- **Provider**: `ProfilingProvider` in `src/velo/observability/profiling.rs`

## Quick Start

### 1. Configuration

Add observability configuration to your `StreamingConfig`:

```rust
use velostream::velo::sql::execution::config::*;

let config = StreamingConfig {
    // ... other config
    tracing: Some(TracingConfig::development()),
    prometheus: Some(PrometheusConfig::default()),
    profiling: Some(ProfilingConfig::development()),
};
```

### 2. Initialize Observability

```rust
use velostream::velo::observability::ObservabilityManager;

let mut obs_manager = ObservabilityManager::new(config).await?;
obs_manager.initialize().await?;

// Use the manager to record metrics
obs_manager.record_sql_query("select", duration, true, 100).await;
obs_manager.record_streaming_operation("ingest", duration, 1000, 500.0).await;
```

### 3. Start Monitoring Stack

```bash
cd grafana/
docker-compose up -d
```

Access:
- **Grafana Dashboard**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Velostream Metrics**: http://localhost:9091/metrics

## Metrics Reference

### SQL Query Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `velo_sql_queries_total` | Counter | Total number of SQL queries executed |
| `velo_sql_query_duration_seconds` | Histogram | SQL query execution time |
| `velo_sql_query_errors_total` | Counter | Total number of SQL query errors |
| `velo_sql_records_processed_total` | Counter | Total records processed by SQL queries |

### Streaming Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `velo_streaming_operations_total` | Counter | Total number of streaming operations |
| `velo_streaming_duration_seconds` | Histogram | Streaming operation duration |
| `velo_streaming_throughput_rps` | Gauge | Current streaming throughput in records/sec |
| `velo_streaming_records_total` | Counter | Total number of records streamed |

### System Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `velo_cpu_usage_percent` | Gauge | Current CPU usage percentage |
| `velo_memory_usage_bytes` | Gauge | Current memory usage in bytes |
| `velo_active_connections` | Gauge | Number of active connections |

## Configuration Options

### TracingConfig

```rust
pub struct TracingConfig {
    pub service_name: String,           // Service identifier
    pub sampling_ratio: f64,            // Trace sampling rate (0.0-1.0)
    pub enable_console_output: bool,    // Enable console logging
    pub otlp_endpoint: Option<String>,  // OpenTelemetry endpoint
}

// Presets
TracingConfig::development()  // Full sampling, console output
TracingConfig::production()   // Low sampling, OTLP export
```

### PrometheusConfig

```rust
pub struct PrometheusConfig {
    pub enable_histograms: bool,    // Enable detailed histograms
    pub port: u16,                  // Metrics endpoint port
    pub metrics_path: String,       // Metrics endpoint path
}

// Presets
PrometheusConfig::default()     // Full metrics on port 9091
PrometheusConfig::lightweight() // Basic metrics only
```

### ProfilingConfig

```rust
pub struct ProfilingConfig {
    pub enable_cpu_profiling: bool,         // Enable CPU profiling
    pub enable_memory_profiling: bool,      // Enable memory profiling
    pub enable_bottleneck_detection: bool,  // Enable bottleneck detection
    pub cpu_threshold_percent: f64,         // CPU warning threshold
    pub memory_threshold_percent: f64,      // Memory warning threshold
    pub output_directory: String,           // Report output directory
    pub retention_days: u32,                // Report retention period
}

// Presets
ProfilingConfig::development()  // All features enabled, low thresholds
ProfilingConfig::production()   // Optimized for production use
```

## Dashboard Features

The Grafana dashboard provides:

### Real-time Monitoring
- **SQL Query Rate**: Queries per second over time
- **Query Duration**: 50th and 95th percentile response times
- **Streaming Operations**: Operations per second and throughput
- **System Resources**: CPU, memory, and connection monitoring

### Performance Analysis
- **Error Rate Tracking**: SQL error percentage
- **Record Processing**: SQL and streaming record rates
- **Bottleneck Detection**: Visual indicators for performance issues

### Alerting (Optional)
Configure alerts based on:
- High error rates (>5%)
- Slow query performance (>1s 95th percentile)
- Resource exhaustion (CPU >85%, Memory >2GB)
- Low throughput (<100 records/sec)

## Performance Profiling

### Automatic Bottleneck Detection

The profiling system automatically detects:

```rust
pub enum BottleneckType {
    HighCpuUsage,           // CPU usage above threshold
    HighMemoryUsage,        // Memory usage above threshold
    SlowQueryExecution,     // SQL queries taking too long
    LowThroughput,          // Streaming throughput below expected
    HighLatency,            // High end-to-end latency
}
```

### Performance Reports

Generated reports include:
- CPU and memory usage analysis
- Query execution patterns
- Throughput statistics
- Bottleneck recommendations

### Profiling Session Example

```rust
// Start a profiling session
let session = profiling_provider.start_session("complex_aggregation").await?;

// Take memory snapshots during operation
session.take_memory_snapshot().await;

// Session automatically cleaned up when dropped
// Report generated with recommendations
```

## App-Level Observability Configuration

### Overview

Velostream supports **app-level observability configuration** through SQL Application annotations. This allows you to define observability settings at the application level rather than repeating configuration for each individual job.

### Three Observability Dimensions

The observability system supports three independent dimensions that can be configured at the app level:

1. **Metrics** (`@observability.metrics.enabled`)
   - Prometheus-compatible metrics collection
   - Query execution times, record rates, error counts
   - Throughput and performance statistics

2. **Tracing** (`@observability.tracing.enabled`)
   - Distributed tracing with OpenTelemetry compatibility
   - End-to-end query execution traces
   - Span hierarchy and latency analysis

3. **Profiling** (`@observability.profiling.enabled`)
   - CPU and memory profiling
   - Automatic bottleneck detection
   - Performance recommendations

### Annotation Syntax

Add observability annotations to the SQL Application header:

```sql
-- SQL Application: My Analytics Platform
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: false

-- Name: Job 1
START JOB job1 AS SELECT * FROM stream1;
```

### Configuration Behavior

- **App-level settings**: Applied to all jobs in the application
- **Per-job settings**: Override app-level settings when explicitly configured
- **Inheritance**: Jobs without explicit settings inherit app-level configuration

Example:

```sql
-- SQL Application: Mixed Requirements
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true

-- This job inherits both metrics and tracing
START JOB standard_job AS SELECT * FROM stream1;

-- This job overrides tracing (keeps metrics from app-level)
START JOB low_overhead_job AS
SELECT * FROM stream2
WITH (observability.tracing.enabled = false);
```

### Implementation Details

**ApplicationMetadata Fields** (src/velostream/sql/app_parser.rs:73-93):
```rust
pub observability_metrics_enabled: Option<bool>,
pub observability_tracing_enabled: Option<bool>,
pub observability_profiling_enabled: Option<bool>,
```

**Configuration Parsing** (src/velostream/sql/app_parser.rs:159-264):
Extracts `@observability.*` annotations from SQL file headers during application parsing.

**Configuration Merging** (src/velostream/server/stream_job_server.rs:1163-1183):
Intelligently merges app-level settings with per-job settings, checking if explicit per-job configuration is already present before injecting app-level defaults.

## Integration Examples

### SQL Query Tracing

```rust
let span = telemetry_provider.start_sql_query_span(
    "SELECT * FROM stream WHERE price > 100", 
    "kafka_orders"
);

// Execute query...
span.set_execution_time(150);  // 150ms
span.set_record_count(42);
span.set_success();
```

### Streaming Operation Monitoring

```rust
let span = telemetry_provider.start_streaming_span("data_ingestion", 1000);

// Process records...
span.set_throughput(500.0);    // 500 records/sec
span.set_processing_time(200); // 200ms
span.set_success();
```

### Metrics Recording

```rust
// Record SQL execution
metrics_provider.record_sql_query(
    "select",
    Duration::from_millis(150),
    true,  // success
    42     // record count
);

// Update system metrics
metrics_provider.update_system_metrics(
    45.5,           // CPU %
    1024*1024*512,  // Memory bytes
    10              // Active connections
);
```

## Troubleshooting

### Common Issues

1. **Metrics Not Appearing**
   - Verify Prometheus is scraping correct endpoint
   - Check Velostream is exposing metrics on configured port
   - Ensure firewall allows access to metrics endpoint

2. **High Memory Usage**
   - Review retention settings in ProfilingConfig
   - Consider reducing histogram bucket count
   - Enable cleanup_old_data() regularly

3. **Dashboard Not Loading**
   - Verify Grafana has access to Prometheus
   - Check dashboard JSON is valid
   - Ensure datasource is properly configured

### Debug Commands

```bash
# Check metrics endpoint directly
curl http://localhost:9091/metrics

# View Prometheus targets
curl http://localhost:9090/api/v1/targets

# Test Grafana datasource
curl -u admin:admin http://localhost:3000/api/datasources
```

## Production Considerations

### Resource Usage
- Metrics collection adds ~2-5% CPU overhead
- Memory usage increases by ~50MB for metrics storage
- Profiling can add 5-10% overhead when enabled

### Security
- Metrics may contain sensitive information (query patterns, data volumes)
- Use authentication for Grafana in production
- Consider network isolation for monitoring components

### Scalability
- Prometheus scales to millions of metrics
- Use recording rules for complex queries
- Consider federation for multi-instance deployments

## Migration Guide

### From Basic Logging
1. Add observability dependencies to Cargo.toml
2. Update configuration to include observability settings
3. Initialize ObservabilityManager in application startup
4. Replace log statements with structured telemetry calls

### Custom Metrics
Add custom business metrics:

```rust
// Register custom metric
let custom_counter = register_int_counter_with_registry!(
    Opts::new("velo_custom_events_total", "Custom business events"),
    &metrics_provider.registry
)?;

// Use in application
custom_counter.inc();
```

## Support

For observability-related issues:
1. Check configuration matches examples in this guide
2. Verify all components are running (Docker Compose status)
3. Review logs in `profiling.output_directory`
4. Use debug-level logging for detailed trace information

The observability system is designed to be lightweight and minimally invasive while providing comprehensive insights into Velostream performance and behavior.