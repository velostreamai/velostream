# FerrisStreams Observability Guide

## Overview

FerrisStreams Phase 4 provides comprehensive observability through distributed tracing, metrics collection, and performance profiling. This guide covers setup, configuration, and usage of the observability infrastructure.

## Architecture

The observability system consists of three main components:

### 1. Distributed Tracing (OpenTelemetry Compatible)
- **Purpose**: Track SQL query execution and streaming operations across components
- **Implementation**: Simplified logging-based spans with structured telemetry data
- **Provider**: `TelemetryProvider` in `src/ferris/observability/telemetry.rs`

### 2. Metrics Collection (Prometheus)
- **Purpose**: Collect and export SQL, streaming, and system metrics
- **Implementation**: Prometheus metrics with histograms, counters, and gauges
- **Provider**: `MetricsProvider` in `src/ferris/observability/metrics.rs`

### 3. Performance Profiling
- **Purpose**: Detect bottlenecks and generate performance reports
- **Implementation**: CPU/memory monitoring with automated report generation
- **Provider**: `ProfilingProvider` in `src/ferris/observability/profiling.rs`

## Quick Start

### 1. Configuration

Add observability configuration to your `StreamingConfig`:

```rust
use ferrisstreams::ferris::sql::execution::config::*;

let config = StreamingConfig {
    // ... other config
    tracing: Some(TracingConfig::development()),
    prometheus: Some(PrometheusConfig::default()),
    profiling: Some(ProfilingConfig::development()),
};
```

### 2. Initialize Observability

```rust
use ferrisstreams::ferris::observability::ObservabilityManager;

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
- **FerrisStreams Metrics**: http://localhost:9091/metrics

## Metrics Reference

### SQL Query Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `ferris_sql_queries_total` | Counter | Total number of SQL queries executed |
| `ferris_sql_query_duration_seconds` | Histogram | SQL query execution time |
| `ferris_sql_query_errors_total` | Counter | Total number of SQL query errors |
| `ferris_sql_records_processed_total` | Counter | Total records processed by SQL queries |

### Streaming Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `ferris_streaming_operations_total` | Counter | Total number of streaming operations |
| `ferris_streaming_duration_seconds` | Histogram | Streaming operation duration |
| `ferris_streaming_throughput_rps` | Gauge | Current streaming throughput in records/sec |
| `ferris_streaming_records_total` | Counter | Total number of records streamed |

### System Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `ferris_cpu_usage_percent` | Gauge | Current CPU usage percentage |
| `ferris_memory_usage_bytes` | Gauge | Current memory usage in bytes |
| `ferris_active_connections` | Gauge | Number of active connections |

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
   - Check FerrisStreams is exposing metrics on configured port
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
    Opts::new("ferris_custom_events_total", "Custom business events"),
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

The observability system is designed to be lightweight and minimally invasive while providing comprehensive insights into FerrisStreams performance and behavior.