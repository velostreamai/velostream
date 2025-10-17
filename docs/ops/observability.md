# Velostream Observability Guide

## Overview

Velostream provides comprehensive observability through distributed tracing, metrics collection, and performance profiling. This guide covers setup, configuration, and usage of the observability infrastructure.

## Quick Start (SQL-Based)

### 1. Enable Observability in Your SQL Application

Add observability annotations to your SQL application header:

```sql
-- SQL Application: My Analytics Platform
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.error_reporting.enabled: true

-- @job_name: data-processor-stream
-- Name: Data Processing Stream
CREATE STREAM data_processor AS
SELECT * FROM input_stream
EMIT CHANGES;

-- @job_name: analytics-stream
-- Name: Analytics Stream
CREATE STREAM analytics AS
SELECT COUNT(*) as record_count FROM input_stream
EMIT CHANGES;
```

### 2. Start the Monitoring Stack

```bash
cd grafana/
docker-compose up -d
```

Access the dashboards:
- **Grafana Dashboard**: http://localhost:3000 (admin/admin)
- **Prometheus Metrics**: http://localhost:9090
- **Raw Metrics Endpoint**: http://localhost:9091/metrics

### 3. View Metrics in Grafana

Your SQL jobs automatically export metrics:
- **Query Performance**: Duration, throughput, error rates
- **Stream Operations**: Records processed per second
- **System Resources**: CPU, memory, connections
- **Errors**: By type, by job, over time

No additional configuration needed! Metrics flow automatically when enabled.

## Configuration: SQL Annotations

Observability is controlled through SQL Application annotations (no code required).

### Annotation Reference

| Annotation | Values | Description |
|-----------|--------|-------------|
| `@observability.metrics.enabled` | `true`, `false` | Enable Prometheus metrics collection |
| `@observability.tracing.enabled` | `true`, `false` | Enable distributed tracing |
| `@observability.profiling.enabled` | `true`, `false` | Enable performance profiling (5-10% overhead) |
| `@observability.error_reporting.enabled` | `true`, `false` | Enable error capture and reporting |

### Example: Production Configuration

```sql
-- SQL Application: Production Trading Platform
-- Version: 1.0.0
-- Description: Real-time financial analytics with comprehensive observability
-- Author: Platform Team
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true
-- @observability.profiling.enabled: false
-- @observability.error_reporting.enabled: true

-- Job-level metrics for Market Data Stream
-- @metric: velo_market_data_total
-- @metric_type: counter
-- @metric_help: "Total market data records processed"
-- @metric_labels: symbol
-- @job_name: market-data-ingestion

-- Name: Market Data Stream
CREATE STREAM market_data AS
SELECT symbol, price, volume FROM market_feed
EMIT CHANGES;

-- Job-level metrics for Price Movement Detection
-- @metric: velo_price_alerts_total
-- @metric_type: counter
-- @metric_help: "Price movement alerts"
-- @metric_labels: symbol, severity
-- @job_name: price-movement-detection

-- Name: Price Movement Detection
CREATE STREAM price_alerts AS
SELECT symbol, ABS(price_change) as movement
FROM market_feed
WHERE ABS(price_change) > 10
EMIT CHANGES;

-- Job-level metrics for Trading Metrics
-- @metric: velo_trade_volume
-- @metric_type: gauge
-- @metric_help: "Trade volume per symbol"
-- @metric_labels: symbol
-- @job_name: trading-metrics-stream

-- Name: Trading Metrics
CREATE STREAM trading_metrics AS
SELECT
    symbol,
    COUNT(*) as trade_count,
    SUM(volume) as total_volume
FROM trades
GROUP BY symbol
EMIT CHANGES;
```

### Per-Job Configuration

Define custom metrics and override app-level settings for specific jobs:

```sql
-- SQL Application: Hybrid Platform
-- @observability.metrics.enabled: true
-- @observability.tracing.enabled: true

-- Job-level metrics
-- @metric: velo_standard_records_total
-- @metric_type: counter
-- @job_name: standard-processing

-- Name: Standard Stream (inherits app-level settings)
CREATE STREAM standard AS
SELECT * FROM stream1
EMIT CHANGES;

-- Job-level metrics with override
-- @metric: velo_lightweight_processed
-- @metric_type: gauge
-- @metric_labels: status
-- @job_name: lightweight-stream

-- Name: Low-Overhead Stream (disable tracing to reduce overhead)
CREATE STREAM lightweight AS
SELECT * FROM stream2
WITH (observability.tracing.enabled = false)
EMIT CHANGES;

-- Job-level metrics for critical monitoring
-- @metric: velo_critical_errors_total
-- @metric_type: counter
-- @metric_help: "Errors in critical stream"
-- @job_name: critical-monitoring

-- Name: Critical Stream (add profiling for analysis)
CREATE STREAM critical AS
SELECT * FROM stream3
WITH (observability.profiling.enabled = true)
EMIT CHANGES;
```

## Monitoring: Metrics Reference

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

### Error Reporting Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `velo_errors_total` | Counter | Total number of errors across all jobs |
| `velo_error_rate` | Gauge | Current error rate (errors per minute) |
| `velo_error_by_type` | Counter | Error count categorized by error type |
| `velo_error_by_job` | Counter | Error count per individual job |
| `velo_serialization_errors_total` | Counter | Total serialization/deserialization errors |
| `velo_sql_parsing_errors_total` | Counter | Total SQL parsing errors |
| `velo_kafka_errors_total` | Counter | Total Kafka connectivity/consumer errors |

## Error Reporting

Velostream provides comprehensive distributed error reporting and tracking through the observability stack.

### Error Reporting Features

**Distributed Error Capture**
- Errors from all jobs and components are collected centrally
- Errors include full context (timestamp, job name, error type, message)
- Error chain information preserved for root cause analysis

**Error Categorization**
- **SQL Parsing Errors**: Invalid SQL syntax or semantic issues
- **Serialization Errors**: Issues with data format conversion (JSON, Avro, Protobuf)
- **Kafka Errors**: Connection failures, consumer lag, producer issues
- **Execution Errors**: Runtime failures during stream processing
- **Configuration Errors**: Invalid settings or missing dependencies

**Error Metrics**
- Total error count across entire application
- Per-job error tracking
- Error rate calculations (errors per time unit)
- Error type distribution

**Error Alerting**
Configure Grafana alerts based on error thresholds:
- High error rate (>10 errors/minute)
- Specific error type spikes (e.g., sudden surge in SQL parsing errors)
- Job-specific errors (critical jobs should have error rate = 0)
- Error type anomalies (unexpected error types appearing)

### Enabling Error Reporting

Error reporting is enabled via SQL annotation (requires metrics):

```sql
-- SQL Application: Production Platform
-- @observability.metrics.enabled: true
-- @observability.error_reporting.enabled: true

-- @job_name: critical-processor-stream
-- Name: Critical Stream
CREATE STREAM critical_processor AS
SELECT * FROM critical_stream
EMIT CHANGES;
```

### Error Reporting Dashboard

The observability dashboard displays:

- **Error Count Timeline**: Historical error trends
- **Error Rate Gauge**: Current errors per minute
- **Error Distribution**: Pie chart of error types
- **Error by Job**: Table showing errors per job
- **Error Details**: Recent error logs with full context

### Error Investigation

When errors occur, the observability stack provides:

1. **Error Metrics**: Know which jobs and error types are affected
2. **Error Context**: Timestamps, job names, and affected records
3. **Error Type Classification**: Understand error categories
4. **Error Trends**: Identify patterns and recurring issues

Example investigation flow:
```
1. Alert triggers: Error rate > 10/minute
2. Check error distribution: 80% serialization errors
3. Filter by job: All errors from "data_ingestion" job
4. Check recent changes: New Protobuf schema version
5. Remediate: Revert schema or update consumer configuration
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

## Troubleshooting

### Common Issues

1. **Metrics Not Appearing**
   - Verify Prometheus is scraping correct endpoint
   - Check Velostream is exposing metrics on configured port
   - Ensure firewall allows access to metrics endpoint

2. **High Memory Usage**
   - Review retention settings if using profiling
   - Consider disabling profiling in production if not needed
   - Check Prometheus retention settings

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

## For Developers: Architecture and Implementation

### System Architecture

The observability system consists of three main components:

**1. Distributed Tracing (OpenTelemetry Compatible)**
- **Purpose**: Track SQL query execution and streaming operations across components
- **Implementation**: Simplified logging-based spans with structured telemetry data
- **Provider**: `TelemetryProvider` in `src/velo/observability/telemetry.rs`

**2. Metrics Collection (Prometheus)**
- **Purpose**: Collect and export SQL, streaming, and system metrics
- **Implementation**: Prometheus metrics with histograms, counters, and gauges
- **Provider**: `MetricsProvider` in `src/velo/observability/metrics.rs`

**3. Performance Profiling**
- **Purpose**: Detect bottlenecks and generate performance reports
- **Implementation**: CPU/memory monitoring with automated report generation
- **Provider**: `ProfilingProvider` in `src/velo/observability/profiling.rs`

### Configuration Options (Rust)

#### TracingConfig

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

#### PrometheusConfig

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

#### ProfilingConfig

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

### Initialization (Rust)

```rust
use velostream::velo::sql::execution::config::*;
use velostream::velo::observability::ObservabilityManager;

// Configure observability
let config = StreamingConfig {
    // ... other config
    tracing: Some(TracingConfig::development()),
    prometheus: Some(PrometheusConfig::default()),
    profiling: Some(ProfilingConfig::development()),
};

// Initialize the observability manager
let mut obs_manager = ObservabilityManager::new(config).await?;
obs_manager.initialize().await?;

// Use the manager to record metrics
obs_manager.record_sql_query("select", duration, true, 100).await;
obs_manager.record_streaming_operation("ingest", duration, 1000, 500.0).await;
```

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

### Integration Examples

#### SQL Query Tracing

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

#### Streaming Operation Monitoring

```rust
let span = telemetry_provider.start_streaming_span("data_ingestion", 1000);

// Process records...
span.set_throughput(500.0);    // 500 records/sec
span.set_processing_time(200); // 200ms
span.set_success();
```

#### Metrics Recording

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

### App-Level Observability Implementation

#### ApplicationMetadata Fields

**File**: `src/velostream/sql/app_parser.rs:73-93`

```rust
pub observability_metrics_enabled: Option<bool>,
pub observability_tracing_enabled: Option<bool>,
pub observability_profiling_enabled: Option<bool>,
```

#### Configuration Parsing

**File**: `src/velostream/sql/app_parser.rs:159-264`

Extracts `@observability.*` annotations from SQL file headers during application parsing.

#### Configuration Merging

**File**: `src/velostream/server/stream_job_server.rs:1163-1183`

Intelligently merges app-level settings with per-job settings, checking if explicit per-job configuration is already present before injecting app-level defaults.

### Custom Metrics (Rust)

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

### Migration Guide (Rust-based)

#### From Basic Logging
1. Add observability dependencies to Cargo.toml
2. Update configuration to include observability settings
3. Initialize ObservabilityManager in application startup
4. Replace log statements with structured telemetry calls

## Support

For observability-related issues:
1. Check configuration matches examples in this guide
2. Verify all components are running (Docker Compose status)
3. Review logs in `profiling.output_directory` (for profiling)
4. Use debug-level logging for detailed trace information

The observability system is designed to be lightweight and minimally invasive while providing comprehensive insights into Velostream performance and behavior.
